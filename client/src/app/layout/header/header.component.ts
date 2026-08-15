import { Component, inject } from '@angular/core';
import { MatIcon } from '@angular/material/icon'; 
import { MatBadge } from '@angular/material/badge';
import { MatButton } from '@angular/material/button';
import { Router, RouterLink, RouterLinkActive } from '@angular/router';
import { CartService } from '../../core/services/cart.service';
import { AccountService } from '../../core/services/account.service';
import { MatMenu, MatMenuItem, MatMenuTrigger } from '@angular/material/menu';
import { MatDivider } from '@angular/material/divider';

@Component({
  selector: 'app-header',
  imports: [
    MatIcon,
    MatButton,
    MatBadge,
    RouterLink,
    RouterLinkActive,
    MatMenu,
    MatMenuTrigger,
    MatDivider,
    MatMenuItem,
  ],
  templateUrl: './header.component.html',
  styleUrl: './header.component.css',
})
export class HeaderComponent {
  public cartService = inject(CartService)
  public acctService = inject(AccountService)
  private router = inject(Router);

  logOut() {
    this.acctService.logout().subscribe({
        next: () => 
          {
            this.acctService.currentUser.set(null)
            this.router.navigateByUrl('/');
          }
    })
  }
}
